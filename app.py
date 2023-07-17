from GeoJSONProcessor import GeoJSONProcessor
from TemperatureDataDownloader import TemperatureDataDownloader

from math import ceil, floor
from datetime import datetime
from collections import defaultdict
import pandas as pd
import time
import argparse

import concurrent.futures
import os

def list_days_of_month(start_date: datetime, end_date: datetime) -> dict:
    """
    List the days of each month within a given date range.

    Args:
        start_date (datetime): Start date.
        end_date (datetime): End date.

    Returns:
        dict: Dictionary with month and days for each month.
        -> {("2023", "01"): ["01", "02", "03", ..., "30"], ("2023", "02"): ["01", "02", "03", ..., "28"]}
    """
    date_dict = defaultdict(list)
    for date in pd.date_range(start_date, end_date):
        date_dict[(str(date.year).zfill(2), str(date.month).zfill(2))].append(str(date.day).zfill(2))

    return date_dict

def main_multiThread(file_path: str):

    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()

    days_of_month_list = list_days_of_month(start_date, end_date)
    # months = ["202301", "202302", ...]
    months = [year + month for year, month in days_of_month_list.keys()]
    
    downloader = TemperatureDataDownloader()
    geojson_processor = GeoJSONProcessor(file_path, months)

    bbox = geojson_processor.bbox
    # The api seems like not accept the bbox with float number.
    bbox_for_downloader = [ceil(bbox[3]), floor(bbox[0]), floor(bbox[1]), ceil(bbox[2])]
    max_threads = os.cpu_count()

    # Download data: I/O-bound -> Using multi-threading to improve the performance.
    
    start_time = time.time()
    # Create a ThreadPoolExecutor with the specified max_workers (limited threads)
    # Note: used limited threads to prevent memory ran out.
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        # Submit tasks to the ThreadPoolExecutor
        futures = []
        for year, month in days_of_month_list:
            days = days_of_month_list[(year, month)]
            future = executor.submit(downloader.download_temperature_data, bbox_for_downloader, year, month, days)
            futures.append(future)
        for future in concurrent.futures.as_completed(futures):
            month = future.result()
            future = executor.submit(geojson_processor.get_monthly_avg_temperature, 
                                     downloader.nc_file_list_by_month, month)
            futures.append(future)
        # Wait for all tasks to complete
        concurrent.futures.wait(futures)
    processing_time = time.time() - start_time

    start_time = time.time()
    # update geoJSON file
    geojson_processor.write_updated_geojson_file()
    write_time = time.time() - start_time

    print(f"{'*' * 10} Multi-Threading Result {'*' * 10}")
    print(f"{'='*50}")
    print(f"* Start Date: {start_date}")
    print(f"* End Date: {end_date}")
    print(str(geojson_processor))
    print(f"{'='*50}")
    print(f"* DownLoad and Processing Time: {processing_time:.2f} seconds")
    print(f"{'='*50}")
    print(f"* Write Time: {write_time:.2f} seconds")
    print(f"{'='*50}")
    print(f"* Total Run Time: {processing_time+write_time:.2f} seconds")


def main_singleThread(file_path):
    """
    Main function to run the single-threaded processing.

    Args:
        file_path (str): Path to the GeoJSON file.

    """
    downloader = TemperatureDataDownloader()
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()
    days_of_month_list = list_days_of_month(start_date, end_date)
    # in order to prevent the concorrent write data to dataframe
    months = [year + month for year, month in days_of_month_list.keys()]
    geojson_processor = GeoJSONProcessor(file_path, months)

    bbox = geojson_processor.bbox
    bbox_for_download = [ceil(bbox[3]), floor(bbox[0]), floor(bbox[1]), ceil(bbox[2])]
    
    download_times = {}
    for year, month in days_of_month_list:
        start_time = time.time()
        days = days_of_month_list[(year, month)]
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

    print(f"{'*' * 10} Single Threading Result {'*' * 10}")
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

    # main_singleThread(args.file_path)
    main_multiThread(args.file_path)

    # main_singleThread('test_features.geojson')
    # main_multiThread('test_features.geojson')
    
