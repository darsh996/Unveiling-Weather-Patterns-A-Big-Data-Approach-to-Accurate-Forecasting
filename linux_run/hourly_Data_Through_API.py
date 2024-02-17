
#Parameters: "T2M,T2MDEW,T2MWET,PS,WS2M,QV2M,RH2M,PRECTOTCORR,WD2M,WS10M,WD10M",

import os
import requests
import json
import csv

# Create a folder to store the output files
output_folder = "D://Project//CDAC-Project//hourly"
os.makedirs(output_folder, exist_ok=True)
print("1")

# create file if request return error
left_file_path = os.path.join("D://Project//CDAC-Project//hourly//left.csv")

# Define base URL
base_site = "https://power.larc.nasa.gov/api/temporal/hourly/point"

interval =  [{"start":"20040101","end":"20081231"},{"start":"20090101","end":"20131231"},{"start":"20140101","end":"20181231"},
                 {"start":"20190101","end":"20231231"}]
print("2")
for time_interval in interval:
    print(time_interval)
    start = time_interval['start']
    end = time_interval['end']
    # Read latitude and longitude values from the CSV file
    with open('D://Project//CDAC-Project//Indian_Cities_Database.csv', 'r') as csvfile:
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
                    "parameters": "QV2M,RH2M,PRECTOTCORR",
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
                    output_file_path = os.path.join(output_folder, f"{city}_{latitude}_{longitude}_{start}_{end}.json")

                    # Write the response to the output file
                    with open(output_file_path, "w") as output_file:
                        json.dump(info, output_file, indent=4)

                    print("Output saved to:", output_file_path)
                else:
                    print("Error:", r.status_code)
                    # Append latitude and longitude to left.csv file
                    with open(left_file_path, 'a', newline='') as left_csvfile:
                        left_writer = csv.writer(left_csvfile)
                        left_writer.writerow([city, latitude, longitude])

            except Exception as e:
                print("Exception occurred:", str(e))
                # Append latitude and longitude to left.csv file
                with open(left_file_path, 'a', newline='') as left_csvfile:
                    left_writer = csv.writer(left_csvfile)
                    left_writer.writerow([city, latitude, longitude])
