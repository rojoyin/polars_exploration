import polars as pl
import requests
import os

url = "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD"
filename = "Electric_Vehicle_Population_Data.csv"


def download_file(url, filename):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(filename, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    print(f"File downloaded successfully as {filename}")

if os.path.exists(filename):
    print(f"{filename} already exists.")
else:
    download_file(url, filename)

electric_vehicle_data = pl.read_csv(filename)
print("First few rows of the data:")
print(electric_vehicle_data.head())
print(electric_vehicle_data.describe())

grouped_by_city = electric_vehicle_data.group_by("City").agg(
    pl.col("VIN (1-10)").count().alias("total_electric_vehicles")
).sort("total_electric_vehicles")

print(grouped_by_city)
