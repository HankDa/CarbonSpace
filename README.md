# Python application for collecting temperature data from CDS for particular locations.

The project implements a program that calculates the monthly 2m temperatures for the centroids of features within a certain time range (20230101 to the present).

## Installation

### Enviroment:

- pip install -r requirements.txt
### CSD API
- refering to https://cds.climate.copernicus.eu/api-how-to

## Structure:
- GeoJSONProcessor.py
  
Take geoJson file as input and get the monthly average temperatures for each features.

- TemperatureDataDownloader.py
  
Download the temperature data by cdsapi and list the downloaded files.

- app.py
  
Run the two module in paralel, print log and output geoJson file. 

## How to use

```
python app.py geojson_path
```

- Result: The program adds the monthly average temperature to each feature's "properties" object with a key in the format YYYYMM and output as a geojson file.

### Tasks:

1. calculate large enough bbox covering all features delivered in the input file
``` 
GeoJSONProcessor._calculate_bbox()
```
This function implemented by geopanda build-in function.

Result: 
```
Bounding Box: [ 9.96947541 57.16153839 10.01127943 57.181209  ]
```

I use [this website](https://geojson.io/#map=14.46/57.17138/9.99038) to check if the bbox covered all features.


2. Use the bbox coordinates to find and download temperature data from CDS (between
01.01.23 and today)

```
TemperatureDataDownloader. download_temperature_data(self, bbox: list, year: str, month: str, days: list[str]):
```
This function utilizes the cdsapi library to make requests and download monthly temperature data instead of the entire dataset. Due to limitations in the API, it has been designed to be easily adaptable for supporting parallel downloads.

3. find centroids for each shape in the input file
```
GeoJSONProcessor._find_features_centroids()
```
This function implemented by geopanda build-in function and save the result as a pd.dataframe for further usage.

Result:
```
              name                  centroid   
0   Inv_Cropland_1  POINT (9.97207 57.16776)
1   Inv_Cropland_2  POINT (9.97586 57.17959)
2  Inv_Grassland_1  POINT (9.98471 57.17426)
3   Inv_Cropland_3  POINT (9.98352 57.16892)
4   Inv_Cropland_4  POINT (9.99985 57.17223)
```

4. use the centroids to identify nearest available temperature datapoint
- Find the nearest location of features' centroid
```
GeoJSONProcessor._identify_nearest_datapoint(self, lat, lon, centroid_idx: int): 

lat_index = (abs(lat - centroid_point.y)).argmin()
lon_index = (abs(lon - centroid_point.x)).argmin()
```

Find the index with the minimum value by subtracting the array from the centroid value (.

Result:
```
Centroids of features (head): 
              name                  centroid                              nearest_point  nearest_idx
0   Inv_Cropland_1  POINT (9.97207 57.16776)  POINT (9.9999999999892 57.20000000000186)  POINT (9 7)
1   Inv_Cropland_2  POINT (9.97586 57.17959)  POINT (9.9999999999892 57.20000000000186)  POINT (9 7)
2  Inv_Grassland_1  POINT (9.98471 57.17426)  POINT (9.9999999999892 57.20000000000186)  POINT (9 7)
3   Inv_Cropland_3  POINT (9.98352 57.16892)  POINT (9.9999999999892 57.20000000000186)  POINT (9 7)
4   Inv_Cropland_4  POINT (9.99985 57.17223)  POINT (9.9999999999892 57.20000000000186)  POINT (9 7)
```
Get daily temperature for each centroid
```
GeoJSONProcessor.get_daily_temperature_by_month(self, nc_file_list_by_month: dict, month: str)
```
Get daily temperatures from downloaded data with nearest location index and convert it to celsius:

```
dataset = nc.Dataset(month+'/'+file_name)
temperature_value = dataset['Temperature_Air_2m_Mean_24h'][0][nearest_idx[1]][nearest_idx[0]] - 273.15
```
Result: 
```
{month_1:{centroid_1:[temp_d1,temp_d2....],centroid_2:[...] }}
```
5. aggregate values to monthly average
```
GeoJSONProcessor.aggregate_monthly_average(self, daily_temperature_by_month: dict, month: str)

```
Sum the daily temperatures and get the average, then return a list of average temperature like:

```
[temperature_centroid_1,temperature_centroid_2,temperature_centroid_3,...]
```
Result:
```
Monthly average temperature: 
               name    202301   202302    202303    202304     202305     202306     202307
0    Inv_Cropland_1  3.533048  3.74259  2.691246  6.911389  11.261137  16.691627  15.291566
1    Inv_Cropland_2  3.533048  3.74259  2.691246  6.911389  11.261137  16.691627  15.291566
2   Inv_Grassland_1  3.533048  3.74259  2.691246  6.911389  11.261137  16.691627  15.291566
3    Inv_Cropland_3  3.533048  3.74259  2.691246  6.911389  11.261137  16.691627  15.291566
4    Inv_Cropland_4  3.533048  3.74259  2.691246  6.911389  11.261137  16.691627  15.291566
5    Inv_Cropland_5  3.533048  3.74259  2.691246  6.911389  11.261137  16.691627  15.291566
6    Inv_Cropland_6  3.533048  3.74259  2.691246  6.911389  11.261137  16.691627  15.291566
7    Inv_Cropland_7  3.533048  3.74259  2.691246  6.911389  11.261137  16.691627  15.291566
8    Inv_Cropland_8  3.533048  3.74259  2.691246  6.911389  11.261137  16.691627  15.291566
9    Inv_Cropland_9  3.533048  3.74259  2.691246  6.911389  11.261137  16.691627  15.291566
10  Inv_Cropland_10  3.533048  3.74259  2.691246  6.911389  11.261137  16.691627  15.291566
11  Inv_Cropland_11  3.533048  3.74259  2.691246  6.911389  11.261137  16.691627  15.291566
```

6. add to each feature’s “properties” object in GeoJSON new key/value. Where key is 6
digits %Y%m and value is average temperature
```
self.gdf.to_file('result.geojson', driver="GeoJSON")  
```

## Performance optimisation
As most of the workload of this program was IO-bound (networking/open file, etc.), I chose multi-threaded processing to improve performance. I implemented parallel download, and once a thread completed its download, it would submit a new task to the ThreadPool, which efficiently downloaded and processed the data in parallel
## Multi-treading performance improvement

```python
********** Single-Threading Result **********
==================================================
* Download Time:
==================================================
Download Time for 202301: 0.82 seconds
Download Time for 202302: 0.59 seconds
Download Time for 202303: 0.40 seconds
Download Time for 202304: 0.59 seconds
Download Time for 202305: 0.66 seconds
Download Time for 202306: 0.37 seconds
Download Time for 202307: 0.51 seconds
-> Total Download Time: 3.95 seconds
==================================================
* Processing Time:
==================================================
Processing Time for 202301: 0.40 seconds
Processing Time for 202302: 0.33 seconds
Processing Time for 202303: 0.41 seconds
Processing Time for 202304: 0.42 seconds
Processing Time for 202305: 0.44 seconds
Processing Time for 202306: 0.32 seconds
Processing Time for 202307: 0.18 seconds
-> Total Processing Time: 2.50 seconds
==================================================
* Write Time: 0.07 seconds
==================================================
* Total Run Time: 6.41 seconds 

********** Multi-Threading Result **********
==================================================
* DownLoad and Processing Time: 3.54 seconds
==================================================
* Write Time: 0.02 seconds
==================================================
* Total Run Time: 3.55 seconds
