import geopandas as gpd

gdf = gpd.read_file('test_features.geojson')

bbox = gdf.total_bounds
# geometry
g = gdf.geometry
print(bbox)

test = [] 

test.append(bbox[0])