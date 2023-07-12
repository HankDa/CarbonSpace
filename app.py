# importing the required module  
import netCDF4 as nc  
import datetime
import cdsapi
import geopandas as gpd
# import cdstoolbox as ct


# defining the path to file  
filePath = 'ad16bf8c-3439-4df4-8f53-0cb5c90e47ea-Temperature-Air-2m-Mean-24h_C3S-glob-agric_AgERA5_20230701_final-v1_area_subset.nc'  
  
# using the Dataset() function  
 

gdf = gpd.read_file('test_features.geojson') 
# minx, miny, maxx, maxy (W,S,E,N)
# looks like the area must be integer.
bbox = gdf.total_bounds

centroids = gdf.geometry.centroid

print(f"bbox: {bbox}")

#TODO: how to iterate through the centroid and find the nearest point. 
print(f"centroid: {centroids}")

dataset = nc.Dataset(filePath)

print(dataset['Temperature_Air_2m_Mean_24h'])

# Get the latitude and longitude variables

lat = len(dataset.variables['lat'])
lon = len(dataset.variables['lon'])
temperature = dataset['Temperature_Air_2m_Mean_24h'][:]
print(f"lat: {lat}")
print(f"lon: {lon}")
print(f"temperature_value: {temperature}")
