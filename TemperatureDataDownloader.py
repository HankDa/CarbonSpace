import cdsapi
import tarfile
import os


class TemperatureDataDownloader:
    """Class to download temperature data using the Climate Data Store (CDS) API."""

    def __init__(self):
        self.cds_client = cdsapi.Client()
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
            return tar_name
        except Exception as e:
            print(f'Error downloading the data: {e}')

    def _list_fileName(self, tar_name: str):
        """
        Extract file names from a .tar.gz file and group them by month.

        Args:
            tar_name (str): Name of the .tar.gz file.

        """
        try: 
            with tarfile.open(tar_name+'.tar.gz', 'r:gz') as tar:
                file_names = tar.getnames()
                tar.extractall(tar_name)
            os.remove(tar_name + '.tar.gz')
            file_names = sorted(file_names)
            self.nc_file_list_by_month[tar_name] = file_names
        except Exception as e:
            print(f'Error extracting the data: {e}')