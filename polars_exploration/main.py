import polars as pl


electric_vehicle_data = pl.read_csv("Electric_Vehicle_Population_Data.csv")

print(electric_vehicle_data.describe())

grouped_by_city = electric_vehicle_data.group_by("City").agg(
    pl.col("VIN (1-10)").count().alias("total_electric_vehicles")
).sort("total_electric_vehicles")

print(grouped_by_city)
