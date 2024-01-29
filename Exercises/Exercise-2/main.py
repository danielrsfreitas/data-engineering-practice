import os
import re
import requests
import pandas as pd

from http import HTTPStatus


"""
Changing a little bit the scope of the exercise because the data source has changed. There is no Last Updated 2022-02-07 14:03

I will use Last Updated 2024-01-19 10:08
"""

url_base = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"

def main():

    response = requests.get(url_base)

    if response.status_code != HTTPStatus.OK:
        raise Exception("No response from the URL")

    # Get line that matches the date
    pattern = r'.*>2024-01-19 10:08.*'
    line = re.search(pattern, response.text).group()

    # Get href (link) that matches the csv to be downloaded
    pattern = r'([0-9]*).csv'
    endpoint = re.search(pattern, line).group()

    response = requests.get(f'{url_base}/{endpoint}')


    os.makedirs('Exercises/Exercise-2/data/', exist_ok=True)

    with open('Exercises/Exercise-2/data/20240119.csv', 'wb') as f:
        f.write(response.content)


    data = pd.read_csv('Exercises/Exercise-2/data/20240119.csv')
    max_temperature = data['HourlyDryBulbTemperature'].max()


    print(f'Max HourlyDryBulbTemperature: {max_temperature}')


if __name__ == "__main__":
    main()
