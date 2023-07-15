import netCDF4 as nc  
import cdsapi
import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import nearest_points
import pandas as pd
from collections import defaultdict
from math import ceil, floor
from datetime import datetime, timedelta
import tarfile
import time
import os
import argparse



def list_days_of_month(start_date: datetime, end_date: datetime) -> dict:
    # Implement the logic to list the days of a month
    date_dict = {}
    current_date = start_date
    while current_date <= end_date:
        year = str(current_date.year).zfill(2)
        month = str(current_date.month).zfill(2)
        day = str(current_date.day).zfill(2)

        if (month, year) not in date_dict:
            date_dict[(month, year)] = []

        date_dict[(month, year)].append(day)

        current_date += timedelta(days=1)
    return date_dict


class TemperatureDataDownloader:
    def __init__(self):
        self.cds_client = cdsapi.Client()
        self.downloaded_tar = []
        # {month-year: [nc_file_names_1, ...], month-year: [nc_file_names_2, ...], ...}
        self.nc_file_list_by_month = {}
    # hide the download log
    def download_temperature_data(self, bbox: list, start_date: datetime, end_date: datetime):
        days_of_month_list = list_days_of_month(start_date, end_date)
        for month, year in days_of_month_list:
            try:
                tar_name = f'{year}{month}'
                self.cds_client.retrieve(
                    'sis-agrometeorological-indicators',
                    {
                        'variable': '2m_temperature',
                        'statistic': '24_hour_mean',
                        'year': year,
                        'month': month,
                        'day': days_of_month_list[(month, year)],
                        'area': [
                            ceil(bbox[3]), floor(bbox[0]), floor(bbox[1]), ceil(bbox[2])
                        ],
                        'format': 'tgz',
                    },
                    f'{tar_name}.tar.gz'
                )
                self._list_fileName(tar_name)
                print(f'Downloaded files: {tar_name}.tar.gz')
            except Exception as e:
                print(f'Error downloading the data: {e}')

    def _list_fileName(self, tar_name):
        with tarfile.open(tar_name+'.tar.gz', 'r:gz') as tar:
            # List all file names in the .tar.gz file
            file_names = tar.getnames()
            # uncompress the .tar.gz file
            tar.extractall(tar_name)
        os.remove(tar_name + '.tar.gz')
        # Group the file names by date
        file_names = sorted(file_names)
        self.nc_file_list_by_month[tar_name] = file_names


class GeoJSONProcessor:
    def __init__(self, file_path: str):
        self.file_path: str = file_path
        self.gdf = self._load_geojson_file()
        self.bbox = self._calculate_bbox()
        self.df_centroids = self._find_features_centroids()
        self.df_monthly_average_temp = pd.DataFrame(self.df_centroids["name"])
    
    # validating the gdf
    def _load_geojson_file(self):
        try: 
            gdf = gpd.read_file(self.file_path) 
        except Exception as e:
            raise Exception(f'Error loading the GeoJSON file: {e}')
        return gdf

    def _calculate_bbox(self):
        # Implement the logic to calculate the bbox covering all features
        # minx, miny, maxx, maxy (W,S,E,N)
        return self.gdf.total_bounds
        
    def _find_features_centroids(self):
        df = pd.DataFrame(columns=['name', 'centroid', 'nearest_point', 'nearest_idx'])
        centroids = self.gdf.geometry.centroid.rename("centroid")
        df = pd.concat([self.gdf['name'], centroids], axis=1)
        return df

    def get_monthly_avgTemperature(self, nc_file_list_by_month, month):
        # daily_temperature_by_month: dict -> {month: {centroid_idx:[tempertature_value, ...], ... }
        daily_temperature_by_month = self._daily_temperature_by_month(nc_file_list_by_month, month)
        # monthly_averages: list -> [avg_temperature_value_01, ...]
        self.df_monthly_average_temp[month] = self._aggregate_monthly_average(daily_temperature_by_month, month)
    
        
    def _update_centroids_with_temperature(self, monthly_averages):
        for centroid_idx in range(len(self.df_centroids.centroid)):
            nearest_idx = self.df_centroids.nearest_idx.iloc[centroid_idx]
            temperature_value = monthly_averages[nearest_idx[1]][nearest_idx[0]]
            self.df_centroids.at[centroid_idx, 'temperature'] = temperature_value
        return self.df_centroids
    
    def _daily_temperature_by_month(self, nc_file_list_by_month: dict, month) -> dict:
        
        daily_temperature_by_month = defaultdict(lambda: defaultdict(list))        
        for file_name in nc_file_list_by_month[month]:
            # the folder named as the month e.g: 01-2023
            dataset = nc.Dataset(month+'/'+file_name)
            # Get the latitude and longitude variables
            lat = dataset.variables['lat'][:]
            lon = dataset.variables['lon'][:]
            
            for centroid_idx in range(len(self.df_centroids.centroid)):
                nearest_idx = self._identify_nearest_datapoint(lat, lon, centroid_idx)
                # Convert the temperature from Kelvin to Celsius
                temperature_value = dataset['Temperature_Air_2m_Mean_24h'][0][nearest_idx[1]][nearest_idx[0]] - 273.15
                daily_temperature_by_month[month][centroid_idx].append(temperature_value)
        return daily_temperature_by_month
        
    # ISSUE: as the Horizontal resolution = 0.1° x 0.1°, the nearest points will be same for each centroid point.
    def _identify_nearest_datapoint(self, lat, lon, centroid_idx: int)-> tuple:
        # Implement the logic to identify the nearest available temperature datapoint
        centroid_point = self.df_centroids.centroid.iloc[centroid_idx]
        lat_index = (abs(lat - centroid_point.y)).argmin()
        lon_index = (abs(lon - centroid_point.x)).argmin()
        self.df_centroids.at[centroid_idx, 'nearest_point'] = Point(lon[lon_index], lat[lat_index])
        # Issue: Must have equal len keys and value when setting with an iterable
        self.df_centroids.at[centroid_idx, 'nearest_idx'] = Point(lon_index, lat_index)
        return (lon_index, lat_index)

    def _aggregate_monthly_average(self, daily_temperature_by_month, month)-> pd.Series:
        monthly_averages = []
        for centroid, temperatures in daily_temperature_by_month[month].items():
            avg_temperature = sum(temperatures) / len(temperatures)
            monthly_averages.append(avg_temperature)
        return monthly_averages


    def _update_geojson_properties(self):
        # Implement the logic to update the GeoJSON properties with temperature data
        self.gdf = pd.merge(self.gdf, self.df_monthly_average_temp, on='name', how='outer')
        print(self.gdf)

    def write_updated_geojson_file(self):
        # Implement the logic to write the updated GeoJSON file
        self._update_geojson_properties()
        self.gdf.to_file('result.geojson', driver="GeoJSON")  
        


def main(file_path):
    downloader = TemperatureDataDownloader()
    geojson_processor = GeoJSONProcessor(file_path)

    bbox = geojson_processor.bbox
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()

    start_time = time.time()
    downloader.download_temperature_data(geojson_processor.bbox, start_date, end_date)
    download_time = time.time() - start_time

    processing_times = {}
    for month in downloader.nc_file_list_by_month:
        start_time = time.time()
        geojson_processor.get_monthly_avg_temperature(downloader.nc_file_list_by_month, month)
        processing_time = time.time() - start_time
        processing_times[month] = processing_time

    start_time = time.time()
    geojson_processor.write_updated_geojson_file()
    write_time = time.time() - start_time

    print("Processing Times:")
    print("=================")
    print(f"Download Time: {download_time:.2f} seconds")
    for month, time in processing_times.items():
        print(f"Processing Time for {month}: {time:.2f} seconds")
    print(f"Write Time: {write_time:.2f} seconds")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process temperature data.')
    parser.add_argument('file_path', type=str, help='Path to the GeoJSON file')
    args = parser.parse_args()

    main(args.file_path)
    
