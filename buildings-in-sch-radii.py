import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

# File paths
initial_csv_file = '/mnt/data/Generalinformationofschools-upload.csv'
buildings_csv_file = '/path/to/your/second/csv_file.csv'  # Replace with actual path

# Read the initial CSV file
df = pd.read_csv(initial_csv_file)

# Assuming the initial CSV has 'longitude' and 'latitude' columns (replace these with the actual column names)
geometry = [Point(xy) for xy in zip(df['Longitude'], df['Latitude'])]  # Adjust column names if necessary

# Convert the DataFrame to a GeoDataFrame
gdf = gpd.GeoDataFrame(df, geometry=geometry)

# Set the CRS to EPSG:4326
gdf.set_crs(epsg=4326, inplace=True)

# Reproject the GeoDataFrame to EPSG:4326
gdf = gdf.to_crs(epsg=4326)

# Create buffers: 1 kilometer and 2 kilometers
gdf['buffer_1km'] = gdf.geometry.buffer(1000)  # Buffer size is in meters
gdf['buffer_2km'] = gdf.geometry.buffer(2000)  # Buffer size is in meters

# Create a difference between the 2km buffer and the 1km buffer to get the 1km-2km range
gdf['buffer_1km_to_2km'] = gdf['buffer_2km'].difference(gdf['buffer_1km'])

# Now read the buildings CSV file
buildings_df = pd.read_csv(buildings_csv_file)

# Create a geometry column from the buildings' latitude and longitude
buildings_geometry = [Point(xy) for xy in zip(buildings_df['Longitude'], buildings_df['Latitude'])]

# Convert the buildings DataFrame to a GeoDataFrame
buildings_gdf = gpd.GeoDataFrame(buildings_df, geometry=buildings_geometry)

# Set the CRS of the buildings GeoDataFrame to EPSG:4326
buildings_gdf.set_crs(epsg=4326, inplace=True)

# Reproject the buildings GeoDataFrame to EPSG:4326
buildings_gdf = buildings_gdf.to_crs(epsg=4326)

# Find buildings within the 0-1km buffer
buildings_within_0_to_1km = gpd.sjoin(buildings_gdf, gdf[['buffer_1km']], how='inner', predicate='within')
buildings_within_0_to_1km['buffer_zone'] = '0-1km'

# Find buildings within the 1km to 2km buffer
buildings_within_1km_to_2km = gpd.sjoin(buildings_gdf, gdf[['buffer_1km_to_2km']], how='inner', predicate='within')
buildings_within_1km_to_2km['buffer_zone'] = '1km-2km'

# Combine both results into the Temp_buffer_table
Temp_buffer_table = pd.concat([buildings_within_0_to_1km, buildings_within_1km_to_2km])

# Identify buildings with the same name as in Temp_buffer_table but not within either buffer
# First, get a list of building names from Temp_buffer_table
buffered_building_names = Temp_buffer_table['Building Name'].unique()

# Find buildings from the original buildings_gdf that are not within either buffer
buildings_not_in_buffer = buildings_gdf[~buildings_gdf['Building Name'].isin(buffered_building_names)]

# For each entry in Temp_buffer_table, count and list buildings not in the buffer
def get_blocks_not_in_buffer(row, buildings_not_in_buffer):
    blocks = buildings_not_in_buffer['Building Name'].tolist()
    return len(blocks), ', '.join(blocks)

# Apply the function to each row in Temp_buffer_table
Temp_buffer_table['Count of blocks not in Pri Sch buffer'], Temp_buffer_table['Blocks not in Pri Sch buffer'] = zip(
    *Temp_buffer_table.apply(lambda row: get_blocks_not_in_buffer(row, buildings_not_in_buffer), axis=1)
)

# Output the Temp_buffer_table
print("Temp_buffer_table:")
print(Temp_buffer_table[['Building Name', 'Latitude', 'Longitude', 'buffer_zone', 'Count of blocks not in Pri Sch buffer', 'Blocks not in Pri Sch buffer']])

# Optionally, save the results to a new CSV file
Temp_buffer_table.to_csv("/mnt/data/Temp_buffer_table_with_additional_columns.csv", index=False)
