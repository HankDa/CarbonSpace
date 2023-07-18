import geopandas as gpd
import pandas as pd
import netCDF4 as nc  
from collections import defaultdict
from shapely.geometry import Point


class GeoJSONProcessor:
    """Class to process GeoJSON files and calculate monthly average temperature."""

    def __init__(self, file_path: str, months: list[str]):
        """
        Initialize GeoJSONProcessor.

        Args:
            file_path (str): Path to the GeoJSON file.

        """
        self.file_path: str = file_path
        self.gdf: gpd.GeoDataFrame = self._load_geojson_file()
        self.bbox: list = self._calculate_bbox()
        self.df_centroids: pd.DataFrame = self._find_features_centroids()
        self.df_monthly_average_temp = pd.DataFrame(self.df_centroids["name"], columns=['name']+months)
    
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
            f"{self.df_monthly_average_temp}\n"
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
        
    def _find_features_centroids(self) -> pd.DataFrame:
        """
        Find the centroids of the features.

        Returns:
            pd.DataFrame: DataFrame containing the feature names and their centroids.

        """
        centroids = self.gdf.geometry.centroid
        df = pd.DataFrame({'name': self.gdf['name'], 'centroid': centroids})

        return df

    def get_monthly_avg_temperature(self, nc_file_list_by_month: dict, month: str):
        """
        Calculate the monthly average temperature.

        Args:
            nc_file_list_by_month (dict): Dictionary containing the list of netCDF file names by month.
            - e.g. {'202301':[nc_file_name,...],'202302':[nc_file_name]}

            month (str): Month for which to calculate the average temperature.
        """
        # daily_temperature_by_month -> {month_1:{centroid_1:[temp_d1,temp_d2....],centroid_2:[...] }}
        daily_temperature_by_month = self._get_daily_temperature_by_month(nc_file_list_by_month, month)
        # self.df_monthly_average_temp[month] -> {month:[avg_temp_centroid_i,...],month_2:[avg_temp_centroid_i,...]} 
        self.df_monthly_average_temp[month] = self._aggregate_monthly_average(daily_temperature_by_month, month)

    def _get_daily_temperature_by_month(self, nc_file_list_by_month: dict, month: str) -> dict:
        """
        Retrieve the daily temperature values for each centroid point in a given month.

        Args:
            nc_file_list_by_month (dict): Dictionary containing the list of netCDF file names by month.
            month (str): Month for which to retrieve the temperature values.

        Returns:
            dict: Dictionary with daily temperature values for each centroid.

        """
        # {month:{controid_id: []}}
        daily_temperature_by_month = defaultdict(lambda: defaultdict(list))        
        
        # iterate the files of the target month
        for file_name in nc_file_list_by_month[month]:
        
            # load temperature dataset 
            dataset = nc.Dataset(month+'/'+file_name)

            # get lat and lon list from the dataset
            lat = dataset.variables['lat'][:]
            lon = dataset.variables['lon'][:]

            # get the daily temperature of each centroid   
            for centroid_idx in range(len(self.df_centroids.centroid)):

                # Find nearest datapoints' index of features' centroid
                nearest_idx = self._identify_nearest_datapoint(lat, lon, centroid_idx)
                
                # Fetch daily temperature data from downloaded data with nearest location index and convert it to celsius
                temperature_value = dataset['Temperature_Air_2m_Mean_24h'][0][nearest_idx[1]][nearest_idx[0]] - 273.15
                
                # Update the dictionary: {month_1:{centroid_1:[temp_d1,temp_d2....],centroid_2:[...] }}
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
        
        Issue:
            the horizontal resolution(0.1° x 0.1°) is not enough to map each centroids with different point.
        """
        centroid_point = self.df_centroids.centroid.iloc[centroid_idx]
        
        # find the nearest lat, lon index for the centroid
        lat_index = (abs(lat - centroid_point.y)).argmin()
        lon_index = (abs(lon - centroid_point.x)).argmin()
        
        # update the information to centorid df
        self.df_centroids.at[centroid_idx, 'nearest_point'] = Point(lon[lon_index], lat[lat_index])
        self.df_centroids.at[centroid_idx, 'nearest_idx'] = Point(lon_index, lat_index)

        return (lon_index, lat_index)

    def _aggregate_monthly_average(self, daily_temperature_by_month: dict, month: str) -> pd.Series:
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
        Merge the input gdf with the monthly average temperature dataframe.

        Returns:
            None.

        """
        self.gdf = pd.merge(self.gdf, self.df_monthly_average_temp, on='name', how='outer')

    def write_updated_geojson_file(self):
        """
        Write the updated GeoJSON file.

        """
        self._update_geojson_properties()
        self.gdf.to_file('result.geojson', driver="GeoJSON")  