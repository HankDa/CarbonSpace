import netCDF4 as nc  

dataset = nc.Dataset('ad16bf8c-3439-4df4-8f53-0cb5c90e47ea-Temperature-Air-2m-Mean-24h_C3S-glob-agric_AgERA5_20230701_final-v1_area_subset.nc 3')
# Get the latitude and longitude variables
lat = dataset.variables['lat'][:]
lon = dataset.variables['lon'][:]

print(lat)
print(lon)