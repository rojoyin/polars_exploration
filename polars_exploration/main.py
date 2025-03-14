import os

import polars as pl
import dask.dataframe as dd

from polars_exploration.commons import download_file, timer_decorator

url = "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD"
filename = "Electric_Vehicle_Population_Data.csv"



if os.path.exists(filename):
    print(f"{filename} already exists.")
else:
    download_file(url, filename)


@timer_decorator
def check_file_with_polars(filename):
    electric_vehicle_data = pl.read_csv(filename)
    print("First few rows of the data:")
    print(electric_vehicle_data.head())
    print(electric_vehicle_data.describe())

    grouped_by_city = electric_vehicle_data.group_by("City").agg(
        pl.col("VIN (1-10)").count().alias("total_electric_vehicles")
    ).sort("total_electric_vehicles")

    print(grouped_by_city)


@timer_decorator
def check_file_with_dask(filename):
    dtypes = {
        '2020 Census Tract': 'float64',
        'Base MSRP': 'float64',
        'Electric Range': 'float64',
        'Legislative District': 'float64',
        'Postal Code': 'float64'
    }
    electric_vehicle_data = dd.read_csv(filename, dtype=dtypes)
    print("First few rows of the data:")
    print(electric_vehicle_data.head())
    print(electric_vehicle_data.describe().compute())

    grouped_by_city = electric_vehicle_data.groupby("City")["VIN (1-10)"].count().compute()
    grouped_by_city = grouped_by_city.sort_values()
    print("\nVehicles per city:")
    print(grouped_by_city)


check_file_with_polars(filename)
check_file_with_dask(filename)