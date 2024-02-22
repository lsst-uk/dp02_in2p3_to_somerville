import aiohttp
import asyncio
import os
import sys
from urllib.parse import urlparse
import aiofiles 
import cProfile
import pandas as pd
import traceback
from contextlib import redirect_stdout

async def fetch(url, session):
    async with session.get(url) as response:
        return await response.read()

async def write_fits_file(file_path, data):
    async with aiofiles.open(file_path, 'wb') as f:
        await f.write(data)
    # with fits.open(file_path, mode='wb') as hdul:
    #     hdul.append(fits.ImageHDU(data=data))
    #     hdul.writeto(file_path, overwrite=True)

async def download_and_write(url, butler_directory, session):
    parsed_url = urlparse(url)
    url_path = parsed_url.path

    directory, filename = os.path.split(url_path)
    local_directory = os.path.join(butler_directory, directory[1:])
    file_path = os.path.join(local_directory, filename)

    #create directories
    os.makedirs(local_directory, exist_ok=True)

    #fetch data
    data = await fetch(url, session)
    #write 
    await write_fits_file(file_path,data)

async def main(urls, butler_directory, max_connections=4):

    connector = aiohttp.TCPConnector(limit=max_connections)
    timeout = aiohttp.ClientTimeout(total=86400)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [download_and_write(url, butler_directory,session) for url in urls]
        await asyncio.gather(*tasks)

if __name__ == "__main__":

    len_argv = len(sys.argv)
    if len_argv < 3:
        print("Usage: python async_http_transfer.py file_with_urls.csv profiler_output.prof")
        sys.exit(1)

    url_file = sys.argv[1]
    profiler_file = sys.argv[2]
    max_connections = 4 if len_argv <= 3 else int(sys.argv[3])
    butler_directory = '/data/butler/dp02/' if len_argv <= 4 else sys.argv[4]
    
    url_file_path = os.path.join(
        os.path.dirname(
            os.path.abspath(__file__)),
        '/csv_files/',
        url_file)
    
    # pd.set_option('display.expand_frame_repr', False)
    pd.set_option('max_colwidth', 100)
    
    try:
        urls = pd.read_csv(url_file, names=['urls'])
        #urls = urls.head()
        url_list = urls.urls.tolist()
        
        profiler = cProfile.Profile()
        profiler.enable()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(url_list, butler_directory, max_connections))
        
        profiler.disable()
        profiler.dump_stats(profiler_file)

        # print(f"Profiler statistics saved to {profiler_file}")

    except FileNotFoundError:
        print("the file "+str(url_file)+" does not exist.")
    except Exception as e:
        print("An exception occurred:", str(e))
        with open('error.log', 'w') as f:
            with redirect_stdout(f):
                traceback.print_exc()
        profiler.disable()
        profiler.dump_stats(profiler_file)

