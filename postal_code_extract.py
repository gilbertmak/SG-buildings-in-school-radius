import requests
import time
import multiprocessing
import pandas as pd

def fetch_building_details(postal_code):
  sg_building_details = []
  page = 1
  
  try:
    #OneMap API currently does not require API key  
    response = requests.get(
          'https://www.onemap.gov.sg/api/common/elastic/search?searchVal={0}&returnGeom=Y&getAddrDetails=Y&pageNum={1}'.format(postal_code, page)).json()
      results = response.get('results', [])
      print(postal_code)
  
  except requests.exceptions.ConnectionError:
      print('Fetching {} failed. Retrying in 2 sec'.format(postal_code))
      time.sleep(2)
      return fetch_building_details(postal_code)  # retry after sleep
  
  for building in results:
      sg_building_details.append(building)
  
  return sg_building_details

def main():
  # Search full range of possible postal codes
  postal_codes = range(0, 1000000)
  postal_codes = ['{0:06d}'.format(p) for p in postal_codes]

  # Configure multiprocessing; change processes variable or remove to use maximum CPU cores
  with multiprocessing.Pool(processes=5) as pool:
      all_building_details = pool.map(fetch_building_details, postal_codes)
  
  # Flatten the list of lists
  flattened_building_details = [detail for sublist in all_building_details for detail in sublist]
  
  # Create a DataFrame from the flattened list of building details
  df = pd.DataFrame(flattened_building_details)
  
  # Save the DataFrame to a CSV file
  df.to_csv('sg_building_details.csv', index=False)
  
  print("CSV file saved as 'sg_building_details.csv' with {} records.".format(len(df)))

# Run with necessary guard prevent recursive creation of subprocesses
if __name__ == '__main__':
    main()
