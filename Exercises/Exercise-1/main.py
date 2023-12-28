import os
import glob
import requests
import shutil
import ssl
import aiohttp
import certifi
import asyncio
import concurrent.futures


download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]




def create_folder(folder_name: str = "downloads"):
    path = rf'{folder_name}'
    if not os.path.exists(path):
        print(f"Create folder {folder_name}")
        os.makedirs(path)

# Without async
def download_artifacts(uris: list, folder_name: str = "downloads"):

    for uri in uris:
        print(f"Retrieve from: {uri}...")
        file_name = uri.split('/')[3]
        try:
            response = requests.get(uri)
            print(f"Save on {file_name}")
            with open(f'{folder_name}/{file_name}', 'wb') as f:
                f.write(response.content)
        except Exception:
            print(f"Failed on request for {uri}")

async def download_artifacts_async(uri: str, folder_name: str = "downloads"):
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    file_name = uri.split('/')[3]
    async with aiohttp.ClientSession() as session:
        async with session.get(uri, ssl=ssl_context) as response:
            print(f"Save on {file_name}")
            with open(f'{folder_name}/{file_name}', 'wb') as f:
                try:
                    await f.write(await response.content.read())
                # TypeError for the integer at the end of the file
                except (aiohttp.ClientResponseError, TypeError):
                    pass



def unzip_files(folder_name: str = "downloads"):

    files = os.listdir(folder_name)
    print(f"Files to unpack {files}")
    for file in files:
        try:
            print(f"Unpack: {file}")
            shutil.unpack_archive(f"{folder_name}/{file}", folder_name)
        except shutil.ReadError as err:
            print(err)

def clean_up_zips(folder_name: str = "downloads"):
    for file in glob.glob(f"{folder_name}/*.zip"):
        print(f"Remove: {file}")
        os.remove(file)


def main(is_async=True):
    # your code here

    folder_name = "downloads"

    create_folder(folder_name=folder_name)
    if is_async:
        for uri in download_uris:
            asyncio.run(download_artifacts_async(uri, folder_name))
    else:
        download_artifacts(download_uris, folder_name=folder_name)
    unzip_files(folder_name=folder_name)
    clean_up_zips(folder_name=folder_name)



if __name__ == "__main__":

    is_async=True
    main(is_async=is_async)
