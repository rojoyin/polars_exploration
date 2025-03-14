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


import time

def timer_decorator(func):
    def wrapper_timer(*args, **kwargs):
        start_time = time.time()
        value = func(*args, **kwargs)
        end_time = time.time()
        run_time = end_time - start_time
        print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        return value
    return wrapper_timer

@timer_decorator
def check_file(filename):
    electric_vehicle_data = pl.read_csv(filename)
    print("First few rows of the data:")
    print(electric_vehicle_data.head())
    print(electric_vehicle_data.describe())

    grouped_by_city = electric_vehicle_data.group_by("City").agg(
        pl.col("VIN (1-10)").count().alias("total_electric_vehicles")
    ).sort("total_electric_vehicles")

    print(grouped_by_city)


check_file(filename)