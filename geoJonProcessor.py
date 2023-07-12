import cdsapi
import geopandas as gpd
import math
from datetime import datetime, timedelta


class TemperatureDataDownloader:
    def __init__(self):
        self.cds_client = cdsapi.Client()

    def download_temperature_data(self, bbox, start_date, end_date):
        # Generate a list of months between start_date and end_date
        months = [str((start_date + timedelta(days=i)).month).zfill(2) for i in range((end_date - start_date).days + 1)]

        # Generate a list of days between start_date and end_date
        days = [str((start_date + timedelta(days=i)).day).zfill(2) for i in range((end_date - start_date).days + 1)]

        self.cds_client.retrieve(
            'sis-agrometeorological-indicators',
            {
                'variable': '2m_temperature',
                'statistic': '24_hour_mean',
                'year': str(start_date.year),
                'month': months,
                'day': days,
                'area': [
                    math.ceil(bbox[3]), math.floor(bbox[0]), math.floor(bbox[1]), math.ceil(bbox[2])
                ],
                'format': 'tgz',
            },
            'download.tar.gz'
        )


class GeoJSONProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.gdf = None
        self.bbox = None
        self.centroids = None

    def load_geojson_file(self):
        self.gdf = gpd.read_file(self.file_path) 
        # validating the gdf

    def calculate_bbox(self):
        # Implement the logic to calculate the bbox covering all features
        # minx, miny, maxx, maxy (W,S,E,N)
        self.bbox = self.gdf.total_bounds
        
    def find_feature_centroids(self):
        # Implement the logic to find centroids for each shape in the GeoJSON file
        self.centroids = self.gdf.geometry.centroid
        
    def identify_nearest_datapoint(self, centroids):
        # Implement the logic to identify the nearest available temperature datapoint
        pass

    def aggregate_monthly_average(self):
        # Implement the logic to aggregate temperature values to monthly average
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
    downloader.download_temperature_data(bbox, start_date, end_date)

    # Process and update the GeoJSON file with temperature data
    geojson_processor.identify_nearest_datapoint(centroids)
    # geojson_processor.aggregate_monthly_average()
    # geojson_processor.update_geojson_properties()
    # geojson_processor.write_updated_geojson_file()
