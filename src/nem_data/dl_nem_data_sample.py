#from conf.config import *
from src.nem_data.download_nem_data_current import download_save_zip_file
import logging

# Configure logging to save to a file
logging.basicConfig(filename=r'C:\Users\n8871191\Downloads\output.log', level=logging.DEBUG)

def download_sample_file():
    url = r'https://www.nemweb.com.au/REPORTS/CURRENT/P5_Reports/PUBLIC_P5MIN_FCAS_REQUIREMENT_202505221045_0000000464357777.zip'
    save_path = r"C:\Users\n8871191\Downloads\sample.zip"
    download_save_zip_file(url, save_path)
    print('Done')

if __name__ == '__main__':
    download_sample_file()
    pass
