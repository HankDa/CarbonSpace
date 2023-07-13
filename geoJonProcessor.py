import netCDF4 as nc  
import cdsapi
import geopandas as gpd
from math import ceil, floor
from datetime import datetime, timedelta
import tarfile


class TemperatureDataDownloader:
    def __init__(self):
        self.cds_client = cdsapi.Client()
        self.downloaded_tar = []
        # {month_1: [nc_file_names_1, ...], month_2: [nc_file_names_2, ...], ...}
        self.nc_file_names_by_month = {}

    def download_temperature_data(self, bbox: list, start_date: datetime, end_date: datetime):
        list_days_of_month = self._list_days_of_month(start_date, end_date)

        for month, year in list_days_of_month:
            try:
                self.cds_client.retrieve(
                    'sis-agrometeorological-indicators',
                    {
                        'variable': '2m_temperature',
                        'statistic': '24_hour_mean',
                        'year': year,
                        'month': month,
                        'day': list_days_of_month[(month, year)],
                        'area': [
                            ceil(bbox[3]), floor(bbox[0]), floor(bbox[1]), ceil(bbox[2])
                        ],
                        'format': 'tgz',
                    },
                    f'download_batch_{year}-{month}.tar.gz'
                )
                self.downloaded_tar.append(f'download_batch_{year}-{month}.tar.gz')
            except Exception as e:
                print(f'Error downloading the data: {e}')

    def _list_days_of_month(self, start_date: datetime, end_date: datetime) -> dict:
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

    def list_fileName(self):
        for tar_name in self.downloaded_tar: 
            with tarfile.open(tar_name, 'r:gz') as tar:
                # List all file names in the .tar.gz file
                file_names = tar.getnames()

            # Group the file names by month
            file_names = sorted(file_names)
            for file_name in file_names:
                # Extract the date portion from the file name
                date_str = file_name.split("_")[3]
                # Convert the date string to a datetime object
                date = datetime.strptime(date_str, "%Y%m%d")

                # Get the month and year as a key
                month_year = date.strftime("%m-%Y")

                # Add the file name to the corresponding month in the dictionary
                if month_year in self.nc_file_names_by_month:
                    self.nc_file_names_by_month[month_year].append(file_name)
                else:
                    self.nc_file_names_by_month[month_year] = [file_name]
        # Print the file names grouped by month
        for month, names in self.nc_file_names_by_month.items():
            print(month)
            for name in names:
                print(name)
            print()


class GeoJSONProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.gdf = None
        self.bbox = None
        self.centroids = None

    def load_geojson_file(self):
        try: 
            self.gdf = gpd.read_file(self.file_path) 
        except Exception as e:
            raise Exception(f'Error loading the GeoJSON file: {e}')
        # validating the gdf

    def calculate_bbox(self):
        # Implement the logic to calculate the bbox covering all features
        # minx, miny, maxx, maxy (W,S,E,N)
        self.bbox = self.gdf.total_bounds
        print(self.bbox)
        
    def find_feature_centroids(self):
        # Implement the logic to find centroids for each shape in the GeoJSON file
        self.centroids = self.gdf.geometry.centroid
    
    # TODO: unzip the file or access it directly? 
    def identify_nearest_datapoint(self, nc_file_names_by_month: dict):
        # Implement the logic to identify the nearest available temperature datapoint
        for month in nc_file_names_by_month:
            for file_name in nc_file_names_by_month[month]:
                dataset = nc.Dataset(file_name)
                # Get the latitude and longitude variables
                lat = dataset.variables['lat'][:]
                lon = dataset.variables['lon'][:]
                # argmin() Return int position of the smallest value in the Series.
                for centroid in centroids:
                    lat_index = (abs(lat - centroid.y)).argmin()
                    lon_index = (abs(lon - centroid.x)).argmin()
                    # temperature of location
                    temperature_value = dataset['Temperature_Air_2m_Mean_24h'][0][lat_index][lon_index]
                    print("Nearest: {} {} Centroid Point: {}, Temperature: {}".format(lat[lat_index], lon[lon_index], centroid, temperature_value))
                # TODO: save the nearest point and temperture to centroid point

    def aggregate_monthly_average(self):
        # Implement the logic to aggregate temperature values to monthly average
        # temperature = dataset['Temperature_Air_2m_Mean_24h'][0][lat_idx][lon_idx]
        pass

    def update_geojson_properties(self):
        # Implement the logic to update the GeoJSON properties with temperature data
        pass

    def write_updated_geojson_file(self):
        # Implement the logic to write the updated GeoJSON file
        pass

    



if __name__ == '__main__':
    # Set up the necessary parameters and instantiate the classes
    file_path = 'test_features.geojson'
    downloader = TemperatureDataDownloader()
    geojson_processor = GeoJSONProcessor(file_path)

    # Load and process the GeoJSON file
    geojson_processor.load_geojson_file()
    geojson_processor.calculate_bbox()
    centroids = geojson_processor.find_feature_centroids()

    # Download temperature data for the calculated bbox
    bbox = geojson_processor.calculate_bbox()
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()
    downloader.download_temperature_data(geojson_processor.bbox, start_date, end_date)
    downloader.list_fileName()

    # Process and update the GeoJSON file with temperature data
    # geojson_processor.identify_nearest_datapoint(downloader.nc_file_names_by_month)
    # geojson_processor.aggregate_monthly_average()
    # geojson_processor.update_geojson_properties()
    # geojson_processor.write_updated_geojson_file()
