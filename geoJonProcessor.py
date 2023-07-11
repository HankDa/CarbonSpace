import cdsapi
import geopandas as gpd
from datetime import datetime, timedelta


class TemperatureDataDownloader:
    def __init__(self, api_key):
        self.cds_client = cdsapi.Client(key=api_key)

    def download_temperature_data(self, bbox, start_date, end_date):
        # Implement the logic to download temperature data from the CDS API
        # TODO: months/area should not be hard coding. And how to handle the downloaded data. 
        c = cdsapi.Client()

        c.retrieve(
            'sis-agrometeorological-indicators',
            {
                'variable': '2m_temperature',
                'statistic': '24_hour_mean',
                'year': '2023',
                'month': [
                    '01', '02', '03',
                    '04', '05', '06',
                    '07',
                ],
                'day': [
                        '01', '02', '03',
                        '04', '05', '06',
                        '07', '08', '09',
                        '10', '11', '12',
                        '13', '14', '15',
                        '16', '17', '18',
                        '19', '20', '21',
                        '22', '23', '24',
                        '25', '26', '27',
                        '28', '29', '30',
                        '31',
                ],
                'area': [
                    bbox[3], bbox[0], bbox[1],
                    bbox[2],
                ],
                'format': 'tgz',
            },
            'download.tar.gz')
        
    def date_period():
        pass
        

class GeoJSONProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.gdf = None
        self.bbox = None
        self.centroids = None

    def load_geojson_file(self):
        self.gdf = gpd.read_file('test_features.geojson') 
        # validating the gdf

    def calculate_bbox(self):
        # Implement the logic to calculate the bbox covering all features
        self.bbox = self.gdf.total_bounds
        
    def find_feature_centroids(self):
        # Implement the logic to find centroids for each shape in the GeoJSON file
        self.centroids = self.gdf.geometry
        
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
    api_key = 'YOUR_CDS_API_KEY'
    downloader = TemperatureDataDownloader(api_key)
    geojson_processor = GeoJSONProcessor('path/to/geojson/file')

    # Load and process the GeoJSON file
    geojson_processor.load_geojson_file()
    geojson_processor.calculate_bbox()
    centroids = geojson_processor.find_feature_centroids()

    # Download temperature data for the calculated bbox
    bbox = geojson_processor.get_calculated_bbox()
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()
    downloader.download_temperature_data(bbox, start_date, end_date)

    # Process and update the GeoJSON file with temperature data
    geojson_processor.identify_nearest_datapoint(centroids)
    geojson_processor.aggregate_monthly_average()
    geojson_processor.update_geojson_properties()
    geojson_processor.write_updated_geojson_file()
