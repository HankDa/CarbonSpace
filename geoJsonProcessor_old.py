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


class TemperatureDataDownloader:
    """Class to download temperature data using the Climate Data Store (CDS) API."""

    def __init__(self):
        self.cds_client = cdsapi.Client()
        self.downloaded_tar = []
        self.nc_file_list_by_month = {}

    def download_temperature_data(self, bbox: list, year: str, month: str, days: list[str]):
        """
        Download temperature data for a specific month and days within that month.

        Args:
            bbox (list): Bounding box coordinates [minx, miny, maxx, maxy].
            year (str): Year.
            month (str): Month.
            days (list[str]): List of days within the month.

        """
        try:
            tar_name = f'{year}{month}'
            self.cds_client.retrieve(
                'sis-agrometeorological-indicators',
                {
                    'variable': '2m_temperature',
                    'statistic': '24_hour_mean',
                    'year': year,
                    'month': month,
                    'day': days,
                    'area': bbox,
                    'format': 'tgz',
                },
                f'{tar_name}.tar.gz'
            )
            self._list_fileName(tar_name)
            print(f'Downloaded files: {tar_name}.tar.gz')

        except Exception as e:
            print(f'Error downloading the data: {e}')

    def _list_fileName(self, tar_name):
        """
        Extract file names from a .tar.gz file and group them by month.

        Args:
            tar_name (str): Name of the .tar.gz file.

        """
        with tarfile.open(tar_name+'.tar.gz', 'r:gz') as tar:
            file_names = tar.getnames()
            tar.extractall(tar_name)
        os.remove(tar_name + '.tar.gz')
        file_names = sorted(file_names)
        self.nc_file_list_by_month[tar_name] = file_names


class GeoJSONProcessor:
    """Class to process GeoJSON files and calculate monthly average temperature."""

    def __init__(self, file_path: str):
        """
        Initialize GeoJSONProcessor.

        Args:
            file_path (str): Path to the GeoJSON file.

        """
        self.file_path: str = file_path
        self.gdf: gpd.GeoDataFrame = self._load_geojson_file()
        self.bbox: list = self._calculate_bbox()
        self.df_centroids: pd.DataFrame = self._find_features_centroids()
        self.df_monthly_average_temp = pd.DataFrame(self.df_centroids["name"])
    
    def __str__(self):
        """
        Return string representation of GeoJSONProcessor.

        Returns:
            str: String representation.

        """
        return (
            f"{'='*50}\n"
            f"* GeoJSON file: {self.file_path}\n"
            f"{'='*50}\n"
            f"* Features (head):\n"
            f"{self.gdf.head()}\n"
            f"{'='*50}\n"
            f"* Bounding Box: {self.bbox}\n"
            f"{'='*50}\n"
            f"* Centroids of features (head): \n"
            f"{self.df_centroids.head()}\n"
            f"{'='*50}\n"
            f"* Monthly average temperature: \n"
            f"{self.df_monthly_average_temp.head()}\n"
        )
        
    def _load_geojson_file(self) -> gpd.GeoDataFrame:
        """
        Load the GeoJSON file.

        Returns:
            gpd.GeoDataFrame: GeoDataFrame containing the loaded GeoJSON file.

        Raises:
            Exception: If there is an error loading the GeoJSON file.

        """
        try: 
            gdf = gpd.read_file(self.file_path) 
        except Exception as e:
            raise Exception(f'Error loading the GeoJSON file: {e}')
        return gdf

    def _calculate_bbox(self) -> list:
        """
        Calculate the bounding box covering all features.

        Returns:
            list: Bounding box coordinates [minx, miny, maxx, maxy].

        """
        return self.gdf.total_bounds
        
    def _find_features_centroids(self):
        """
        Find the centroids of the features.

        Returns:
            pd.DataFrame: DataFrame containing the feature names and their centroids.

        """
        df = pd.DataFrame(columns=['name', 'centroid', 'nearest_point', 'nearest_idx'])
        centroids = self.gdf.geometry.centroid.rename("centroid")
        df = pd.concat([self.gdf['name'], centroids], axis=1)
        return df

    def get_monthly_avg_temperature(self, nc_file_list_by_month: dict, month: str):
        """
        Calculate the monthly average temperature.

        Args:
            nc_file_list_by_month (dict): Dictionary containing the list of netCDF file names by month.
            month (str): Month for which to calculate the average temperature.

        """
        daily_temperature_by_month = self._daily_temperature_by_month(nc_file_list_by_month, month)
        self.df_monthly_average_temp[month] = self._aggregate_monthly_average(daily_temperature_by_month, month)

    def _daily_temperature_by_month(self, nc_file_list_by_month: dict, month) -> dict:
        """
        Retrieve the daily temperature values for each centroid point in a given month.

        Args:
            nc_file_list_by_month (dict): Dictionary containing the list of netCDF file names by month.
            month (str): Month for which to retrieve the temperature values.

        Returns:
            dict: Dictionary with daily temperature values for each centroid.

        """
        daily_temperature_by_month = defaultdict(lambda: defaultdict(list))        
        for file_name in nc_file_list_by_month[month]:
            dataset = nc.Dataset(month+'/'+file_name)
            lat = dataset.variables['lat'][:]
            lon = dataset.variables['lon'][:]
            
            for centroid_idx in range(len(self.df_centroids.centroid)):
                nearest_idx = self._identify_nearest_datapoint(lat, lon, centroid_idx)
                temperature_value = dataset['Temperature_Air_2m_Mean_24h'][0][nearest_idx[1]][nearest_idx[0]] - 273.15
                daily_temperature_by_month[month][centroid_idx].append(temperature_value)
        return daily_temperature_by_month
        
    def _identify_nearest_datapoint(self, lat, lon, centroid_idx: int) -> tuple:
        """
        Identify the nearest available temperature datapoint for a centroid.

        Args:
            lat (np.ndarray): Array of latitude values.
            lon (np.ndarray): Array of longitude values.
            centroid_idx (int): Index of the centroid.

        Returns:
            tuple: Indices of the nearest datapoint (lon_index, lat_index).

        """
        centroid_point = self.df_centroids.centroid.iloc[centroid_idx]
        lat_index = (abs(lat - centroid_point.y)).argmin()
        lon_index = (abs(lon - centroid_point.x)).argmin()
        self.df_centroids.at[centroid_idx, 'nearest_point'] = Point(lon[lon_index], lat[lat_index])
        self.df_centroids.at[centroid_idx, 'nearest_idx'] = Point(lon_index, lat_index)
        return (lon_index, lat_index)

    def _aggregate_monthly_average(self, daily_temperature_by_month, month) -> pd.Series:
        """
        Calculate the monthly average temperature for each centroid.

        Args:
            daily_temperature_by_month (dict): Dictionary with daily temperature values for each centroid.
            month (str): Month for which to calculate the average.

        Returns:
            pd.Series: Series with monthly average temperature values.

        """
        monthly_averages = []
        for centroid, temperatures in daily_temperature_by_month[month].items():
            avg_temperature = sum(temperatures) / len(temperatures)
            monthly_averages.append(avg_temperature)
        return monthly_averages


    def _update_geojson_properties(self):
        """
        Update the GeoJSON properties with temperature data.

        """
        self.gdf = pd.merge(self.gdf, self.df_monthly_average_temp, on='name', how='outer')

    def write_updated_geojson_file(self):
        """
        Write the updated GeoJSON file.

        """
        self._update_geojson_properties()
        self.gdf.to_file('result.geojson', driver="GeoJSON")  
        

def list_days_of_month(start_date: datetime, end_date: datetime) -> dict:
    """
    List the days of each month within a given date range.

    Args:
        start_date (datetime): Start date.
        end_date (datetime): End date.

    Returns:
        dict: Dictionary with month and days for each month.

    """
    date_dict = defaultdict(list)
    for date in pd.date_range(start_date, end_date):
        date_dict[(str(date.year).zfill(2), str(date.month).zfill(2))].append(str(date.day).zfill(2))

    return date_dict


def main_singleThread(file_path):
    """
    Main function to run the single-threaded processing.

    Args:
        file_path (str): Path to the GeoJSON file.

    """
    downloader = TemperatureDataDownloader()
    geojson_processor = GeoJSONProcessor(file_path)

    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()
    days_of_month_list = list_days_of_month(start_date, end_date)
    bbox = geojson_processor.bbox
    bbox_for_download = [ceil(bbox[3]), floor(bbox[0]), floor(bbox[1]), ceil(bbox[2])]
    
    download_times = {}
    for year, month in days_of_month_list:
        days = days_of_month_list[(year, month)]
        start_time = time.time()
        downloader.download_temperature_data(bbox_for_download, year, month, days)
        download_time = time.time() - start_time
        download_times[(year, month)] = download_time

    processing_times = {}
    for month in downloader.nc_file_list_by_month:
        start_time = time.time()
        geojson_processor.get_monthly_avg_temperature(downloader.nc_file_list_by_month, month)
        processing_time = time.time() - start_time
        processing_times[month] = processing_time

    start_time = time.time()
    geojson_processor.write_updated_geojson_file()
    write_time = time.time() - start_time

    print(f"{'*' * 10} Result {'*' * 10}")
    print(f"{'='*50}")
    print(f"* Start Date: {start_date}")
    print(f"* End Date: {end_date}")
    print(str(geojson_processor))
    print(f"{'='*50}")
    print(f"* Download Time:")
    print(f"{'='*50}")
    total_download_time = 0
    for (year, month), download_time in download_times.items():
        total_download_time += download_time
        print(f"Download Time for {month}-{year}: {download_time:.2f} seconds")
    print(f"-> Total Download Time: {total_download_time:.2f} seconds")
    print(f"{'='*50}")
    print(f"* Processing Time:")
    print(f"{'='*50}")
    total_processing_time = 0
    for month, processing_time in processing_times.items():
        total_processing_time += processing_time
        print(f"Processing Time for {month}: {processing_time:.2f} seconds")
    print(f"-> Total Processing Time: {total_processing_time:.2f} seconds")
    print(f"{'='*50}")
    print(f"* Write Time: {write_time:.2f} seconds")
    print(f"{'='*50}")
    print(f"* Total Run Time: {total_download_time+total_processing_time+write_time:.2f} seconds")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process temperature data.')
    parser.add_argument('file_path', type=str, help='Path to the GeoJSON file')
    args = parser.parse_args()

    main_singleThread(args.file_path)

    # main_singleThread('test_features.geojson')
    
