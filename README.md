# Python application for collecting temperature data from CDS for particular locations.

The project implement a program to calculate the monthly 2m temperatures for the centroid of features for certain time range(20230101 to present).

## Installation

### Enviroment:

- pip install -r requirements.txt
### CSD API
- refering to https://cds.climate.copernicus.eu/api-how-to

## How to use
- python app.py geojson_path

The monthly average temperature will be add to each feature’s “properties” object with YYYYMM key. 

## Result

```python
* Monthly average temperature: 
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

## Multi-treading performance improvement

```python
********** Single-Threading Result **********
==================================================
* Download Time:
==================================================
Download Time for 01-2023: 0.90 seconds
Download Time for 02-2023: 0.59 seconds
Download Time for 03-2023: 0.42 seconds
Download Time for 04-2023: 0.63 seconds
Download Time for 05-2023: 0.64 seconds
Download Time for 06-2023: 0.36 seconds
Download Time for 07-2023: 0.29 seconds
-> Total Download Time: 3.84 seconds
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
