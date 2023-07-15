import netCDF4 as nc  
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

file_path = 'test_features.geojson'
# Read the GeoDataFrame from file
gdf = gpd.read_file(file_path)
dataset = nc.Dataset("01-2023/b4e0ce94-a87c-4d89-8dd8-8604d203fdc9-Temperature-Air-2m-Mean-24h_C3S-glob-agric_AgERA5_20230101_final-v1_area_subset.nc")
# Calculate centroids
centroids = gdf.geometry.centroid.rename('centroid')
# Create a new DataFrame with 'name' and centroid points

def identify_nearest_datapoint(lat, lon, centroid_idx):
    # Implement the logic to identify the nearest available temperature datapoint
    centroid_point = df.centroid.iloc[centroid_idx]
    lat_index = (abs(lat - centroid_point.y)).argmin()
    lon_index = (abs(lon - centroid_point.x)).argmin()
    df.at[centroid_idx,'nearest_point'] = Point(lon[lon_index], lat[lat_index])
    df.at[centroid_idx,'nearest_idx'] = (lon_index, lat_index)
    return df.nearest_idx.iloc[centroid_idx]

df = pd.DataFrame(columns=['name', 'centroid', 'nearest_point', 'nearest_idx'])

df["name"] = gdf["name"]
df["centroid"] = centroids
# Print the DataFrame

lat = dataset.variables['lat'][:]
lon = dataset.variables['lon'][:]
print(lat)
print(lon)
for centroid_idx in range(len(df.centroid)):
    near_idx = identify_nearest_datapoint(lat, lon, centroid_idx)
    temperature_value = dataset['Temperature_Air_2m_Mean_24h'][0][int(near_idx[1])][int(near_idx[0])]
    print(temperature_value)
    
print(df)




